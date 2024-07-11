import { DefaultFooter } from '@ant-design/pro-components';
import React from 'react';

const Footer: React.FC = () => {
  return (
    <DefaultFooter
      style={{
        background: 'none',
      }}
      links={[
        {
          key: '© Databend',
          title: '© Databend',
          href: 'https://github.com/datafuselabs/databend',
          blankTarget: true,
        },
      ]}
    />
  );
};

export default Footer;
